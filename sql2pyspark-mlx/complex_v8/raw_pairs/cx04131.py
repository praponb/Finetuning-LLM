from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("premium") >= 1042.5) & (F.year(F.col("end_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj_2"),
    )
)

annotated = (
    base_policys
    .filter(F.col("product") == "AUTO")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_adj",
        "premium_adj_2",
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
    )
)

result = (
    annotated
    .groupingSets(
        [["holder_id", "status"], ["holder_id"], []],
        "holder_id", "status",
    )
    .agg(F.round(F.avg("premium"), 2).alias("avg_premium"))
)
