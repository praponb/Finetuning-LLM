from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").asc_nulls_first(), "id")
w2 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("start_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") > "2023-07-01"))
    .select(
        "*",
        F.last_day("end_date").alias("end_date_month_end"),
        F.add_months("start_date", 3).alias("start_date_plus_months"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-04-19"))) & (F.col("plan") == "enterprise"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "end_date_month_end",
        "start_date_plus_months",
        "rn",
        (F.datediff("end_date_month_end", "start_date") / 7).alias("weeks_start_date_to_end_date_month_end"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "start_date",
        "status",
        F.first("weeks_start_date_to_end_date_month_end", ignorenulls=True).over(w2).alias("first_known_weeks_start_date_to_end_date_month_end"),
        F.first("weeks_start_date_to_end_date_month_end", ignorenulls=True).over(w3).alias("first_known_weeks_start_date_to_end_date_month_end_2"),
        F.first("weeks_start_date_to_end_date_month_end", ignorenulls=True).over(w4).alias("first_known_weeks_start_date_to_end_date_month_end_3"),
    )
)

result = (
    scored
    .select(
        "id",
        "start_date",
        "status",
        "first_known_weeks_start_date_to_end_date_month_end_2",
        "first_known_weeks_start_date_to_end_date_month_end_3",
    )
    .orderBy(F.col("status").desc_nulls_last())
)
