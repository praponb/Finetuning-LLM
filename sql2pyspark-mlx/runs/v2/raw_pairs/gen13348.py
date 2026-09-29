from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "DE") & (F.length("supplier_name") < 5))
    .select(
        "id",
        "rating",
        "supplier_name",
        "country",
        F.when(F.col("rating") >= 4.3, "High").when(F.col("rating") >= 2.2, "Medium").otherwise("Low").alias("rating_band"),
        F.col("rating").cast("int").alias("rating_int"),
    )
)
