from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 110)
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.concat("status", F.lit(" - "), "plan").alias("status_plan"),
    )
)

scored = cleaned.withColumn("seq_no", F.row_number().over(w))

filtered = (
    scored
    .filter(F.col("start_date") > F.lit("2023-06-04").cast("date"))
    .select("*", F.round("seq_no", 1).alias("seq_no_rounded"))
)

base = (
    filtered
    .filter(F.col("start_date") >= F.lit("2023-09-24").cast("date"))
    .select("*", F.length("plan").alias("plan_length"))
)

result = (
    base
    .select("id", "start_date", "status", "seq_no", "seq_no_rounded", "plan_length")
    .orderBy("seq_no", F.col("plan_length").desc())
)
