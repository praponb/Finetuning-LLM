from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units").alias("store_sale_units"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.sale_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

joined = (
    latest_match
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.rating"), F.col("d.country"), F.col("d.supplier_name"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("quantity") >= 235, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        F.when(F.col("store_sale_units") >= 43, "HIGH").otherwise("LOW").alias("store_sale_units_tier"),
    )
    .agg(
        F.stddev_samp("price").alias("price_stddev"),
        F.max(F.when(F.col("country") == "UK", F.col("cost"))).alias("uk_max_cost"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
    )
    .filter(F.col("us_rows") > 1)
    .orderBy(F.col("us_rows").desc_nulls_last())
)
