from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("supplier_name").like("P%"))
    .select(
        "id",
        "supplier_name",
        "country",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") != 126), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "supplier_name_title",
        "city_filled",
        F.concat("supplier_name", F.lit("/"), "country").alias("supplier_name_country"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.supplier_name_title",
        "s.supplier_name_country",
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

kept_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").like("C%")), "left_anti")
)

result = (
    kept_suppliers
    .groupBy(F.date_trunc("MONTH", "product_launch_date").alias("activity_month"))
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("event_count") >= 1)
)
