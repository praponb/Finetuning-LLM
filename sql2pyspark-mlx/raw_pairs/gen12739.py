from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-10-27").cast("date")) & (F.col("plan") == "premium"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        (F.datediff(F.current_date(), "start_date") / 365).alias("years_since_start_date"),
        F.concat_ws("/", "status", "plan").alias("status_plan_key"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
