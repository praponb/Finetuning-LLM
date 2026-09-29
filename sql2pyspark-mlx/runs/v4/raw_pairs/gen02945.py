from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("cost", "id")
w_part = Window.partitionBy("brand")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

with_dims = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.lead("cost", 1).over(w_hist).alias("next_cost"),
        F.corr("cost", "stock_qty").over(w_part).alias("corr_cost_stock_qty"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.revenue").alias("store_sale_revenue"))
)

matched_keys = (
    latest_match
    .select("category", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.size("tags") >= 2)
        .select("category", "supplier_id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "city",
        F.when(F.col("store_sale_revenue") >= 3300, "HIGH").otherwise("LOW").alias("store_sale_revenue_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("next_cost")).otherwise(0)).alias("acme_next_cost"),
        F.stddev_samp("price").alias("price_stddev"),
    )
    .orderBy("london_count")
)
