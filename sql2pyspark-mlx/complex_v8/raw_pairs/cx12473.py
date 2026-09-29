from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_claim_id_by_claim = Window.partitionBy("holder_id", "claim_id").orderBy("claim_date", "id", "claim_id")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_claim_id_by_claim_2 = Window.partitionBy("holder_id", "claim_id").orderBy("claim_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_product = Window.partitionBy("status", "product")
w_claim_date_rank_product_by_c = Window.partitionBy("claim_date_rank", "product").orderBy("claim_reported_at", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_rank_product_by_c_2 = Window.partitionBy("claim_date_rank", "product").orderBy("claim_reported_at", "id", "claim_id")

base = (
    df_policies
    .filter((F.col("product") != "LIFE") & (F.col("premium") <= 2945.9))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

line_level = (
    base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.hour(F.col("c.reported_at")).between(10, 16)), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.end_date_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

sequenced = (
    line_level
    .select(
        "*",
        F.rank().over(w_holder_id_claim_id_by_claim).alias("claim_date_rank"),
        F.round(F.col("claim_claimed_amount") - F.avg("claim_claimed_amount").over(w_holder_id), 2).alias("claim_claimed_amount_vs_holder_id_avg"),
        F.first("claim_date", ignorenulls=True).over(w_holder_id_claim_id_by_claim_2).alias("first_known_claim_date"),
        F.sum("claim_claimed_amount").over(w_holder_id_claim_id_by_claim_2).alias("running_claim_claimed_amount"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.col("claim_date_rank") / F.nullif(F.sum("claim_date_rank").over(w_status_product), F.lit(0)), 4).alias("claim_date_rank_share_of_status_product"),
        F.first("claim_date", ignorenulls=True).over(w_claim_date_rank_product_by_c).alias("first_known_claim_date_2"),
        F.max("claim_date_rank").over(w_status_product).alias("status_product_max_claim_date_rank"),
        F.ntile(10).over(w_claim_date_rank_product_by_c_2).alias("claim_reported_at_decile"),
        F.datediff(F.col("first_known_claim_date"), F.lag("first_known_claim_date").over(w_claim_date_rank_product_by_c_2)).alias("days_since_prev_first_known_claim_date"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "claim_id",
        "claim_date_rank_share_of_status_product",
        "claim_claimed_amount",
        "claim_claimed_amount_vs_holder_id_avg",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_date_rank_share_of_status_product", "claim_claimed_amount", "claim_claimed_amount_vs_holder_id_avg"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
