from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.8, 4.0) | F.col("country").isin(["UK", "TH"]))
    .select(
        "id",
        "supplier_name",
        "rating",
        "city",
        F.when(F.col("rating") >= 3.5, "High").when(F.col("rating") >= 2.5, "Medium").otherwise("Low").alias("rating_band"),
        F.concat("country", F.lit(" "), "city").alias("country_city"),
    )
    .orderBy(F.col("rating").desc())
)
