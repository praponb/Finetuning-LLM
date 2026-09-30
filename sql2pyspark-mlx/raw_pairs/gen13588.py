from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .select("id", "brand", "price", "cost")
    .unpivot(
        ids=["id", "brand"],
        values=["price", "cost"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
