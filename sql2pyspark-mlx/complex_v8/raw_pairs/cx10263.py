from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_product_end_date_month_by_pr = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_policies
    .filter(F.col("premium") > 2096.6)
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

sequenced = (
    cleaned
    .withColumn("premium_share_of_status", F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4))
    .withColumn("last_known_start_date", F.last("start_date", ignorenulls=True).over(w_product_end_date_month_by_pr))
)

qualified = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_share_of_status",
        "last_known_start_date",
        F.abs(F.col("premium_share_of_status") - 0.6).alias("premium_share_of_status_distance"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
    .orderBy(F.col("last_known_start_date").asc(), F.col("id").desc())
)
