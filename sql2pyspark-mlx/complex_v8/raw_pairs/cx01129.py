from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_product_by_sta = Window.partitionBy(F.year("end_date"), "product").orderBy("start_date", "id")
w_product_status_by_premium = Window.partitionBy("product", "status").orderBy("premium", "id")
w_status = Window.partitionBy("status")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_policies
    .filter(F.col("premium").between(1042.8, 3521.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

ranked = (
    filtered
    .select(
        "*",
        (F.col("premium") - F.lag("premium").over(w_end_date_year_product_by_sta)).alias("premium_change"),
        (F.col("premium") - F.lag("premium").over(w_product_status_by_premium)).alias("premium_change_2"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.max("premium").over(w_all_by_premium_running).alias("max_premium_so_far"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
    .filter(F.col("premium") < 3817.5)
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isNotNull()) & (F.col("p.holder_name").isNotNull()), "left_anti")
)

qualified = (
    with_activity_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id") >= 4) & (F.col("c.status").isNotNull()), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "premium_change",
        "premium_change_2",
        "premium_vs_status_avg",
        "rows_in_status",
        F.round(F.col("premium_vs_status_avg") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_vs_status_avg_per_premium"),
    )
    .orderBy(F.col("holder_id").asc())
)
