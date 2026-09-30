from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.round("rating", 2).alias("rating_rounded"))
)

result = (
    cleaned
    .select(
        "id",
        "city",
        F.col("rating_rounded").alias("RATING_ROUNDED_VAL"),
        F.col("rating").alias("RATING_VAL"),
    )
    .unpivot(
        ids=["id", "city"],
        values=["RATING_ROUNDED_VAL", "RATING_VAL"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
