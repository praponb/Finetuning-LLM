from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_month = Window.orderBy(F.col("start_date_month").desc(), "id")
w_start_date_month_to_date = Window.partitionBy(F.to_date("start_date_month"))

filtered_policys = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2026) & (F.col("premium").between(1042.8, 3719.7)))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

screened = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 33975.8) & (F.col("c.claim_type") != "fire"), "left_semi")
)

with_activity = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") < 20843.1) & (F.col("c.claimed_amount") > 12726.7), "left_semi")
)

windowed = (
    with_activity
    .select(
        "*",
        F.ntile(4).over(w_all_by_start_date_month).alias("start_date_month_quartile"),
        F.count(F.lit(1)).over(w_start_date_month_to_date).alias("rows_in_start_date_month_to_date"),
    )
)

result = (
    windowed
    .groupBy("end_date")
    .pivot("status", ["CANCELLED", "LAPSED"])
    .agg(F.max("premium_filled"))
    .orderBy("end_date")
)
