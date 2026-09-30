from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("product") == "AUTO")
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label_2"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

scored = (
    base_policys
    .filter(F.col("status_product_label_2").isNotNull())
    .select(
        "id",
        "premium",
        "end_date",
        "days_since_end_date",
        F.when(F.col("days_since_end_date") >= 525, "large").when(F.col("days_since_end_date") >= 245, "medium").otherwise("small").alias("days_since_end_date_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

result = (
    scored
    .select(
        "id",
        F.col("days_since_end_date").alias("days_since_end_date_v"),
        F.col("end_date_dayofweek").alias("m_end_date_dayofweek"),
    )
    .unpivot(
        ids=["id"],
        values=["days_since_end_date_v", "m_end_date_dayofweek"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
