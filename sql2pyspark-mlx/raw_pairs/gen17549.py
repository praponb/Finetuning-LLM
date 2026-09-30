from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment", "city").orderBy(F.col("age").asc_nulls_first())

result = (
    df_customers
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
    )
)
