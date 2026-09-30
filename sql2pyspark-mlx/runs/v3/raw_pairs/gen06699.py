from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("brand")
    .intersect(
        df_products
        .filter(F.col("category").isin(["Fashion", "Sports", "Grocery"]))
        .select("brand")
    )
)
