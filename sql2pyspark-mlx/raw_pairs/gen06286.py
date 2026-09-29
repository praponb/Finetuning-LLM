from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("city").orderBy("product_launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.month("product_launch_date"), "city").orderBy("rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy(F.col("rating_bucket").desc_nulls_first())
w9 = Window.partitionBy("city").orderBy("city_min_supplier_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city").orderBy(F.col("city_min_supplier_name_length").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.length("supplier_name").alias("supplier_name_length"))
)

metrics = (
    base
    .withColumn("rating_bucket", F.ntile(3).over(w))
    .withColumn("city_min_supplier_name_length", F.min("supplier_name_length").over(w2))
    .withColumn("country_min_supplier_name_length", F.min("supplier_name_length").over(w3))
    .withColumn("first_known_supplier_name_length", F.first("supplier_name_length", ignorenulls=True).over(w4))
)

cleaned = (
    metrics
    .filter(F.col("supplier_name_length").between(55, 79))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "supplier_name_length",
        "rating_bucket",
        "city_min_supplier_name_length",
        "country_min_supplier_name_length",
        "first_known_supplier_name_length",
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.supplier_name_length",
        "s.rating_bucket",
        "s.city_min_supplier_name_length",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.stock_qty"),
    )
)

scored = (
    latest_match
    .withColumn("running_city_min_supplier_name_length", F.sum("city_min_supplier_name_length").over(w6))
    .withColumn("running_supplier_name_length", F.sum("supplier_name_length").over(w_running))
    .withColumn("rating_bucket_dense_rank", F.dense_rank().over(w8))
    .withColumn("last_known_city_min_supplier_name_length", F.last("city_min_supplier_name_length", ignorenulls=True).over(w9))
    .withColumn("city_min_supplier_name_length_bucket", F.ntile(3).over(w10))
)

result = (
    scored
    .groupBy(F.date_trunc("MONTH", "product_launch_date").alias("activity_month"))
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy("activity_month")
)
