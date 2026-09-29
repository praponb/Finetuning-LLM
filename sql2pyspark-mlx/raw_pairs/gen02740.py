from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("days_start_date_to_end_date").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy("start_date", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-05-22").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "plan", "start_date")
)

metrics = (
    ranked
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

result = (
    metrics
    .groupBy("customer_id")
    .agg(
        F.avg("event_seq").alias("avg_event_seq"),
        F.sum(F.when(F.col("plan") == "standard", F.col("event_seq")).otherwise(0)).alias("standard_event_seq"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
    )
)
