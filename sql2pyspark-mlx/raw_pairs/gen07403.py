from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("customer_id", "city").orderBy(F.col("start_date").desc(), "id")

valid = (
    df_subscriptions
    .filter(F.year("start_date") == 2024)
    .withColumn("status_group", F.when(F.col("status").isin(["EXPIRED", "CANCELLED"]), "Group A").otherwise("Other"))
    .withColumn("status_upper", F.upper("status"))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("row_num") >= 1)
    .withColumn("months_end_date_to_start_date", F.round((F.datediff("start_date", "end_date") / 30), 1))
    .withColumn("plan_length", F.length("plan"))
)

enriched = (
    cleaned
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.city"))
)

latest_subscriptions = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "start_date", "status", "row_num", "plan_length", "city")
)

pivoted = (
    latest_subscriptions
    .groupBy("status")
    .pivot("city", ["Bangkok", "Singapore", "New York"])
    .agg(F.max("plan_length").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "status",
        F.col("Bangkok_value_sum").alias("bangkok_value_sum"),
        F.col("Bangkok_n").alias("bangkok_n"),
        F.col("Singapore_value_sum").alias("singapore_value_sum"),
        F.col("Singapore_n").alias("singapore_n"),
        F.col("New York_value_sum").alias("new_york_value_sum"),
        F.col("New York_n").alias("new_york_n"),
    )
)

result = (
    pivoted
    .select(
        "status",
        "bangkok_value_sum",
        "singapore_value_sum",
        "singapore_n",
        "new_york_n",
    )
    .orderBy(F.col("new_york_n").asc_nulls_last(), F.col("bangkok_value_sum").desc())
)
