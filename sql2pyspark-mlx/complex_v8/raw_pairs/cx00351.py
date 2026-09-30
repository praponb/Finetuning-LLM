from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_ties = Window.orderBy("end_date")
w_product_status_by_premium_ti = Window.partitionBy("product", "status").orderBy("premium")
w_holder_id = Window.partitionBy("holder_id")

recent_policys = (
    df_policies
    .filter((F.col("premium") > 1541.8) & (F.col("status").isin("ACTIVE", "CANCELLED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HOME", "LIFE"), "group_a").when(F.col("product") == "HEALTH", "group_b").otherwise("other").alias("product_group"),
    )
)

with_activity = (
    recent_policys
    .crossJoin(recent_policys.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

metrics = (
    with_activity
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "product_group",
        F.dense_rank().over(w_all_by_end_date_ties).alias("end_date_dense_rank"),
        F.round(F.cume_dist().over(w_product_status_by_premium_ti), 3).alias("premium_cume_dist"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
    )
)

result = (
    metrics
    .groupBy("end_date_dense_rank", "product_group")
    .pivot("status", ["LAPSED", "EXPIRED", "ACTIVE", "CANCELLED"])
    .agg(F.max("premium"))
    .orderBy("end_date_dense_rank", "product_group")
)
