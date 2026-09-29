from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_2_month_hol = Window.partitionBy(F.date_trunc("month", "holder_joined_on_2"), "holder_id").orderBy("holder_joined_on_2", "id")
w_holder_joined_on_2_month_hol_2 = Window.partitionBy(F.date_trunc("month", "holder_joined_on_2"), "holder_id").orderBy("holder_joined_on_2")
w_holder_joined_on_month = Window.partitionBy(F.month("holder_joined_on"))

valid_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

joined_holder = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.year(F.col("p.joined_on")) == 2025)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_label",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

joined_holder_2 = (
    joined_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("L%")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.start_date_label",
        "s.holder_joined_on",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on_2"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

sequenced = (
    joined_holder_2
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "start_date_label",
        "holder_joined_on",
        "holder_name",
        "holder_joined_on_2",
        "holder_risk_score",
        F.ntile(3).over(w_holder_joined_on_2_month_hol).alias("holder_joined_on_2_tercile"),
        F.dense_rank().over(w_holder_joined_on_2_month_hol_2).alias("holder_joined_on_2_dense_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_joined_on_month), F.lit(0)), 4).alias("premium_share_of_holder_joined_on_month"),
        F.min("premium").over(w_holder_joined_on_month).alias("holder_joined_on_month_min_premium"),
    )
)

result = (
    sequenced
    .select("product")
    .distinct()
)
