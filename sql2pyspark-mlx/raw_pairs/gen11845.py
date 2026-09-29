from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("signup_date", "id")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "country",
        "signup_date",
        "age",
        (F.col("age") - F.lag("age", 1, 0).over(w)).alias("age_change"),
        F.round((F.col("age") / F.sum("age").over(w2)), 3).alias("age_share"),
    )
)
