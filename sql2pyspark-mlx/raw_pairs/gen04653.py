from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "JP") | F.col("city").isin(["New York", "London", "Chiang Mai"]))
    .select(
        "id",
        "country",
        "supplier_name",
        F.round((F.col("rating") * 1.1), 2).alias("rating_adjusted"),
    )
    .orderBy("id", "rating_adjusted")
)
