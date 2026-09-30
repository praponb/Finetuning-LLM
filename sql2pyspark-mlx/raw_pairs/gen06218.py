from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 100), 2).alias("adjusted_budget"),
        F.coalesce("location", F.lit("NONE")).alias("location_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "location",
        F.col("adjusted_budget").alias("ADJUSTED_BUDGET_VAL"),
        F.col("budget").alias("BUDGET_VAL"),
    )
    .unpivot(
        ids=["id", "location"],
        values=["ADJUSTED_BUDGET_VAL", "BUDGET_VAL"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
