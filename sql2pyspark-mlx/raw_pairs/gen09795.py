from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.col("country") != "UK")
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        "country",
        (F.col("credit_limit") - F.lag("credit_limit", 1).over(w)).alias("credit_limit_change"),
        F.count("*").over(w2).alias("country_count"),
    )
)
