from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-24").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .select("id", "store_id", "greatest_units_revenue", "revenue_per_unit")
    .unpivot(
        ids=["id", "store_id"],
        values=["greatest_units_revenue", "revenue_per_unit"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
