from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product = Window.partitionBy("status", "product")
w_product_status_by_premium = Window.partitionBy("product", "status").orderBy(F.col("premium").desc(), "id")
w_product_status_by_premium_ru = Window.partitionBy("product", "status").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_id_product = Window.partitionBy("claim_id", "product")
w_claim_id_holder_id_by_end_da = Window.partitionBy("claim_id", "holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_product_by_prev_pr = Window.partitionBy("holder_id", "product").orderBy("prev_premium", "id", "claim_id")

valid_policys = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2026-01-20").cast("date"), F.lit("2027-01-03").cast("date"))) & (F.col("holder_id") >= 10))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

ranked = (
    valid_policys
    .select(
        "*",
        F.max("premium").over(w_status_product).alias("status_product_max_premium"),
        F.round(F.percent_rank().over(w_product_status_by_premium), 3).alias("premium_percent_rank"),
        F.sum("premium").over(w_product_status_by_premium_ru).alias("running_premium"),
        F.lag("premium").over(w_product_status_by_premium).alias("prev_premium"),
    )
)

exploded_detail_claims = (
    ranked.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.prev_premium",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_windows = (
    exploded_detail_claims
    .select(
        "id",
        "holder_id",
        "product",
        "prev_premium",
        "claim_id",
        "claim_date",
        "claim_approved_amount",
        F.round(F.col("prev_premium") / F.nullif(F.sum("prev_premium").over(w_claim_id_product), F.lit(0)), 4).alias("prev_premium_share_of_claim_id_product"),
        F.sum("claim_approved_amount").over(w_claim_id_product).alias("claim_id_product_sum_claim_approved_amount"),
        F.first("claim_approved_amount").over(w_claim_id_holder_id_by_end_da).alias("first_claim_approved_amount"),
        F.lag("prev_premium", 3).over(w_holder_id_product_by_prev_pr).alias("prev3_prev_premium"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["holder_id", "product"], ["holder_id"]],
        "holder_id", "product",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("claim_approved_amount") / F.nullif(F.sum("prev_premium"), F.lit(0)), 3).alias("claim_approved_amount_to_prev_premium_ratio"),
        F.round(F.sum("prev_premium_share_of_claim_id_product"), 2).alias("total_prev_premium_share_of_claim_id_product"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "row_count",
        "claim_approved_amount_to_prev_premium_ratio",
        "total_prev_premium_share_of_claim_id_product",
    )
)
