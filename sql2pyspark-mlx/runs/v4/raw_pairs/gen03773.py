from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/checkout", "/home"]) & F.col("page").isin(["/home", "/checkout", "/product"]))
    .select("id", "duration_sec")
    .orderBy("id", "duration_sec")
)
