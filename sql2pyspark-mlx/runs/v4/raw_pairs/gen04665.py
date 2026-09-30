from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id")
w2 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("adjusted_credit_limit", "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w6 = Window.partitionBy("city", "country")
w7 = Window.partitionBy("city").orderBy("adjusted_credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country").orderBy(F.col("adjusted_credit_limit_bucket_share").asc_nulls_last(), "id")
w9 = Window.partitionBy("country").orderBy(F.col("signup_date").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-08-27").cast("date")) & F.col("segment").isNotNull() & F.col("age").between(31, 38))
    .select(
        "*",
        F.when(F.col("credit_limit") >= 14200, "High").when(F.col("credit_limit") >= 6200, "Medium").otherwise("Low").alias("credit_limit_band"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

valid = (
    cleaned
    .filter((F.col("signup_date") >= F.lit("2024-02-14").cast("date")) & (F.col("country") == "SG"))
    .select(
        "*",
        F.concat_ws("-", "name", "city").alias("name_city_key"),
        F.round((F.col("credit_limit") * 0.85), 2).alias("adjusted_credit_limit"),
    )
)

enriched = (
    valid
    .withColumn("age_bucket", F.ntile(4).over(w))
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("adjusted_credit_limit_bucket", F.ntile(4).over(w3))
    .withColumn("country_min_adjusted_credit_limit", F.min("adjusted_credit_limit").over(w4))
    .withColumn("age_bucket_2", F.ntile(10).over(w5))
)

enriched_customers = (
    enriched
    .select(
        "*",
        F.round((F.col("adjusted_credit_limit_bucket") / F.sum("adjusted_credit_limit_bucket").over(w6)), 4).alias("adjusted_credit_limit_bucket_share"),
        F.last("age_bucket_2", ignorenulls=True).over(w7).alias("last_known_age_bucket_2"),
    )
)

ranked = (
    enriched_customers
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.rank().over(w9))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "credit_limit_band",
        "city_filled",
        "adjusted_credit_limit",
        "running_credit_limit",
    )
    .orderBy(F.col("segment").asc_nulls_first())
)
