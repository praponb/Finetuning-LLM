from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-09-08"))) & (F.col("brand").eqNullSafe(F.lit("Umbrella"))))
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.supplier_id",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 7, 8, 9])), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "supplier_id")
    .intersectAll(
        df_products
        .filter(F.col("brand").isin(["Initech", "Stark"]))
        .select("id", "supplier_id")
    )
)

verified = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.city"))
)

grouped = (
    with_dims
    .groupBy("city")
    .agg(
        F.countDistinct("brand").alias("distinct_brands"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
)

result = grouped.select("city", "distinct_brands", "london_count")
