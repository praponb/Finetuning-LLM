from pyspark.sql import functions as F

avg_price = df_products.filter(F.col("brand").isin(["Initech", "Acme"])).agg(F.avg("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") >= avg_price)
    .select("id", "price", "brand", "launch_date")
    .orderBy("brand", F.col("id").desc())
)
