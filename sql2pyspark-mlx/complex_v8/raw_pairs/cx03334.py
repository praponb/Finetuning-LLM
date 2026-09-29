from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_joined_on_mont = Window.partitionBy("status", F.date_trunc("month", "holder_joined_on")).orderBy("holder_joined_on")
w_all_by_holder_joined_on_movi = Window.orderBy("holder_joined_on", "id").rowsBetween(-6, Window.currentRow)
w_all_by_holder_joined_on = Window.orderBy("holder_joined_on", "id")
w_status_holder_joined_on_mont_2 = Window.partitionBy("status", F.date_trunc("month", "holder_joined_on")).orderBy("holder_joined_on", "id")
w_status_product_by_holder_ris = Window.partitionBy("status", "product").orderBy("holder_risk_score", "id")
w_holder_region_holder_joined = Window.partitionBy("holder_region", F.month("holder_joined_on")).orderBy("start_date_plus_1m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_region = Window.partitionBy("product", "holder_region")
w_holder_region = Window.partitionBy("holder_region")

valid = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("premium").between(1132.1, 4688.7)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.add_months(F.col("start_date"), 1).alias("start_date_plus_1m"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.start_date_plus_1m",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "start_date_plus_1m",
        "holder_region",
        "holder_risk_score",
        F.rank().over(w_status_holder_joined_on_mont).alias("holder_joined_on_rank"),
        F.max("premium").over(w_all_by_holder_joined_on_movi).alias("premium_max_7"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_all_by_holder_joined_on)).alias("days_since_prev_holder_joined_on"),
        F.round(F.cume_dist().over(w_status_holder_joined_on_mont_2), 3).alias("holder_joined_on_cume_dist"),
        F.lead("start_date_plus_1m").over(w_status_product_by_holder_ris).alias("next_start_date_plus_1m"),
        F.lag("holder_joined_on").over(w_status_product_by_holder_ris).alias("prev_holder_joined_on"),
        F.last("premium", ignorenulls=True).over(w_holder_region_holder_joined).alias("last_known_premium"),
        F.sum("premium").over(w_product_holder_region).alias("product_holder_region_sum_premium"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_region), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_region"),
    )
)

result = (
    ranked
    .filter(F.col("holder_risk_score_share_of_holder_region") >= 0.5)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "holder_region",
        "holder_risk_score",
        "holder_joined_on_rank",
        "premium_max_7",
        "days_since_prev_holder_joined_on",
        "next_start_date_plus_1m",
        "prev_holder_joined_on",
        "last_known_premium",
        "holder_risk_score_share_of_holder_region",
        F.year(F.col("start_date_plus_1m")).alias("start_date_plus_1m_year"),
        F.least(F.col("holder_risk_score"), F.col("holder_joined_on_rank")).alias("least_holder_risk_score_holder_joined_on_rank"),
    )
    .orderBy(F.col("product").desc(), F.col("premium_max_7").asc())
)
