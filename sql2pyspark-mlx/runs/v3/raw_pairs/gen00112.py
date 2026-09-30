from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Sports")
    .groupBy("category")
    .pivot("brand", ["Globex", "Acme"])
    .agg(F.sum("cost"))
    .orderBy("category")
)
