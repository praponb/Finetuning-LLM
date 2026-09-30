from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-07-13").cast("date"))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)

result = (
    valid
    .select("id", "product_id", "revenue", "units_filled")
    .unpivot(
        ids=["id", "product_id"],
        values=["revenue", "units_filled"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
