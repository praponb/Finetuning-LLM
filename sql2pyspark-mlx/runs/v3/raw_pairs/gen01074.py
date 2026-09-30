from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Acme", "Umbrella"]))
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Acme", "Stark"])
    .agg(F.sum("price"))
    .orderBy("category")
)
