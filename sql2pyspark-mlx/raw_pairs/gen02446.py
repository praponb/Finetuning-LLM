from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "supplier_name",
        F.when(F.col("rating") >= 3.7, "High").when(F.col("rating") >= 2.0, "Medium").otherwise("Low").alias("rating_band"),
        F.col("rating").cast("int").alias("rating_int"),
    )
    .orderBy(F.col("rating_int").desc())
)
