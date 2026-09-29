from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "city").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "end_date"), "customer_id").orderBy("monthly_fee", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("dedup_rank").desc(), "id")

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.end_date",
        "src.monthly_fee",
        F.col("p.city"),
        F.col("p.name"),
    )
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter((F.col("end_date") >= F.lit("2023-10-02").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .withColumn("end_date_year", F.year("end_date"))
    .withColumn("city_clean", F.coalesce("city", F.lit("NONE")))
)

with_windows = (
    filtered
    .select(
        "id",
        "city",
        "name",
        "dedup_rank",
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_2_prior"),
        F.lag("monthly_fee", 1, 0).over(w3).alias("prev_monthly_fee"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_3"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "city",
        "name",
        "dedup_rank",
        "monthly_fee_avg_2_prior",
        "prev_monthly_fee",
        "monthly_fee_avg_3",
        "row_num",
        F.when(F.col("name").rlike("^(Grace Kim|  Carol Diaz)$"), "EXTENDED").otherwise("PRIORITY").alias("name_zone"),
    )
    .orderBy(F.col("city").desc())
)
