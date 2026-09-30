from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_first_reported_at_mon = Window.partitionBy("status", F.date_trunc("month", "first_reported_at")).orderBy(F.col("first_reported_at").desc(), "reported_at_month", "reported_at_dayofweek", "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_dayofweek_status = Window.partitionBy("reported_at_dayofweek", "status")
w_status_first_reported_at_mon_2 = Window.partitionBy("status", F.date_trunc("month", "first_reported_at")).orderBy(F.col("first_reported_at").desc())
w_first_reported_at_year_by_st = Window.partitionBy(F.year("first_reported_at")).orderBy(F.col("stddev_reported_at_dayofweek").desc(), "reported_at_month", "reported_at_dayofweek", "status").rowsBetween(-6, Window.currentRow)
w_status_by_first_reported_at = Window.partitionBy("status").orderBy(F.col("first_reported_at").desc(), "reported_at_month", "reported_at_dayofweek", "status").rowsBetween(-3, Window.currentRow)

valid = (
    df_claims
    .filter((F.col("claimed_amount") >= 5774.6) & (F.col("status").isin("REJECTED", "OPEN")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
    )
)

per_group = (
    valid
    .groupBy(
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        "reported_at_dayofweek",
        "status",
    )
    .agg(
        F.round(F.stddev("reported_at_dayofweek"), 2).alias("stddev_reported_at_dayofweek"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.min("reported_at").alias("first_reported_at"),
    )
)

ranked = (
    per_group
    .select(
        "reported_at_month",
        "reported_at_dayofweek",
        "status",
        "status_list",
        F.count(F.lit(1)).over(w_status_first_reported_at_mon).alias("running_count"),
        F.count(F.lit(1)).over(w_reported_at_dayofweek_status).alias("rows_in_reported_at_dayofweek_status"),
        F.round(F.percent_rank().over(w_status_first_reported_at_mon_2), 3).alias("first_reported_at_percent_rank"),
        F.sum("stddev_reported_at_dayofweek").over(w_first_reported_at_year_by_st).alias("stddev_reported_at_dayofweek_sum_7"),
        F.round(F.avg("reported_at_dayofweek").over(w_status_by_first_reported_at), 2).alias("reported_at_dayofweek_avg_4"),
        F.max("reported_at_dayofweek").over(w_reported_at_dayofweek_status).alias("reported_at_dayofweek_status_max_reported_at_day"),
    )
)

scored = (
    ranked
    .withColumn("status_slug", F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")))
)

result = (
    scored
    .groupBy("reported_at_dayofweek", "reported_at_month")
    .pivot("status", ["OPEN", "APPROVED"])
    .agg(F.sum("reported_at_dayofweek_status_max_reported_at_day").alias("total"), F.count("reported_at_dayofweek_status_max_reported_at_day").alias("n"))
)
