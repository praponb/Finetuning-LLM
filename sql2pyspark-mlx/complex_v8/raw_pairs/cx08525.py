from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("status") == "CANCELLED"))
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

summary = (
    base
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "holder_id",
    )
    .agg(
        F.min("premium").alias("min_premium"),
        F.sum(F.when(~F.col("product").isin("AUTO", "LIFE"), 1).otherwise(0)).alias("n_product_not_in_auto_life"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("premium"))).alias("premium_status_expired"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    summary
    .select(
        "start_date_month",
        "holder_id",
        F.col("n_rows").alias("n_rows_v"),
        F.col("n_product_not_in_auto_life").alias("m_n_product_not_in_auto_life"),
    )
    .unpivot(
        ids=["start_date_month", "holder_id"],
        values=["n_rows_v", "m_n_product_not_in_auto_life"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
