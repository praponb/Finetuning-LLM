from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-11").cast("date"))
    .select(
        "*",
        F.round("units", 2).alias("units_rounded"),
        F.date_sub("sale_date", 86).alias("sale_date_minus_86d"),
    )
)

result = (
    valid
    .select("id", "product_id", "units_rounded", "revenue")
    .unpivot(
        ids=["id", "product_id"],
        values=["units_rounded", "revenue"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
