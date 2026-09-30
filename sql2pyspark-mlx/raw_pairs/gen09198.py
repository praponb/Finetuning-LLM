from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_times_years_since_start_date_plus_years_since_start_date").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-09-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        (F.datediff(F.current_date(), "start_date") / 365).alias("years_since_start_date"),
        F.when(F.col("status").isin(["ACTIVE", "PAUSED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

valid = (
    filtered
    .filter((F.col("end_date") > F.lit("2023-08-26").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_times_years_since_start_date", (F.col("monthly_fee") * F.col("years_since_start_date")))
)

cleaned = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("years_since_start_date") < 65))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "years_since_start_date",
        "status_group",
        "monthly_fee_times_years_since_start_date",
        (F.col("monthly_fee_times_years_since_start_date") + F.col("years_since_start_date")).alias("monthly_fee_times_years_since_start_date_plus_years_since_start_date"),
    )
)

valid_subscriptions = (
    cleaned
    .filter((F.col("end_date") > "2024-07-01") & (F.col("end_date") > F.lit("2023-07-11").cast("date")))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status_group",
        "monthly_fee_times_years_since_start_date",
        "monthly_fee_times_years_since_start_date_plus_years_since_start_date",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

cleaned_subscriptions = (
    valid_subscriptions
    .filter((F.col("end_date") > F.lit("2023-06-05").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "monthly_fee_times_years_since_start_date",
        "monthly_fee_times_years_since_start_date_plus_years_since_start_date",
        "monthly_fee_int",
        F.col("monthly_fee_times_years_since_start_date").cast("int").alias("monthly_fee_times_years_since_start_date_int"),
    )
)

latest = (
    cleaned_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "monthly_fee_times_years_since_start_date",
        "monthly_fee_int",
        "monthly_fee_times_years_since_start_date_int",
    )
    .orderBy("monthly_fee_times_years_since_start_date_int")
)
