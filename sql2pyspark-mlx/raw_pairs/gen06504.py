from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-09-05").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("years_since_start_date", (F.datediff(F.current_date(), "start_date") / 365))
)

result = (
    filtered
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "years_since_start_date",
        F.min("years_since_start_date").over(w_part).alias("last7_min_years_since_start_date"),
        F.max("years_since_start_date").over(w_part).alias("last7_max_years_since_start_date"),
        F.sum("years_since_start_date").over(w_part).alias("last7_sum_years_since_start_date"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
    )
)
