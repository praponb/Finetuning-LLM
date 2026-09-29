from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_holder_joined_on = Window.partitionBy("product").orderBy(F.col("holder_joined_on").desc(), "id")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_status_group_by_holder_risk = Window.partitionBy("status_group").orderBy("holder_risk_score", "id").rowsBetween(-3, -1)
w_product_by_holder_joined_on_2 = Window.partitionBy("product").orderBy(F.col("holder_joined_on").desc())
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium = Window.orderBy("premium", "id")
w_status_product = Window.partitionBy("status", "product")

scoped = (
    df_policies
    .filter(F.col("premium") >= 1541.8)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.status_group",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.rank().over(w_product_by_holder_joined_on).alias("holder_joined_on_rank"),
        F.min("premium").over(w_end_date_month).alias("end_date_month_min_premium"),
        F.round(F.avg("premium").over(w_status_group_by_holder_risk), 2).alias("premium_avg_3_prior"),
        F.size(F.collect_set("status").over(w_end_date_month)).alias("distinct_status_in_end_date_month"),
        F.round(F.percent_rank().over(w_product_by_holder_joined_on_2), 3).alias("holder_joined_on_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_premium_running).alias("running_count"),
        F.lead("holder_joined_on").over(w_all_by_premium).alias("next_holder_joined_on"),
        F.lead("holder_joined_on", 2).over(w_all_by_premium).alias("next2_holder_joined_on"),
        F.sum("premium").over(w_end_date_month).alias("end_date_month_sum_premium"),
        F.count(F.lit(1)).over(w_status_product).alias("rows_in_status_product"),
        F.sum("premium").over(w_all_by_premium_running).alias("running_premium"),
    )
)

first_rows = metrics.filter(F.col("holder_joined_on_rank") == 1)

result = (
    first_rows
    .select("distinct_status_in_end_date_month", "holder_joined_on")
    .distinct()
    .orderBy(
        F.col("holder_joined_on").desc(),
        F.col("distinct_status_in_end_date_month").desc(),
    )
)
