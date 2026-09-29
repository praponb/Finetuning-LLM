from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & (F.col("rating") < 3.1))
    .select(
        "city",
        "supplier_name",
        F.when(F.col("rating") >= 4.5, "High").when(F.col("rating") >= 1.7, "Medium").otherwise("Low").alias("rating_band"),
    )
    .orderBy("rating_band")
)
