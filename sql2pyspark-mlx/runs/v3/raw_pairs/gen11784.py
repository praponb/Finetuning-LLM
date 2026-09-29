from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .filter(~F.col("country").isin(["SG", "US", "JP"]))
    .select(
        "id",
        "country",
        "signup_date",
        "segment",
        "age",
        F.first("segment").over(w).alias("first_segment"),
        F.round((F.col("age") / F.sum("age").over(w2)), 3).alias("age_share"),
    )
    .orderBy(F.col("id").desc())
)
