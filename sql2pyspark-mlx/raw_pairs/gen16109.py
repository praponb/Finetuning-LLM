from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-06").cast("date"))
    .withColumn("units_whole", F.ceil("units"))
    .withColumn("units_rounded", F.round("units", 2))
)

result = (
    base
    .select("id", "store_id", "units_rounded", "revenue")
    .unpivot(
        ids=["id", "store_id"],
        values=["units_rounded", "revenue"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
