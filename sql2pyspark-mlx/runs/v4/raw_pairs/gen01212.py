from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("signup_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("country", "city").orderBy(F.col("dedup_rank").desc())

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-09-17").cast("date")) & F.col("city").isNotNull())
    .select("*", F.initcap(F.trim("city")).alias("city_title"))
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (~F.col("c.status").isin(["EXPIRED", "CANCELLED", "PAUSED"])), "left_anti")
)

deduped_customers = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped_customers
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "city_title",
        "dedup_rank",
        "row_num",
    )
    .orderBy(F.col("city").desc())
)
