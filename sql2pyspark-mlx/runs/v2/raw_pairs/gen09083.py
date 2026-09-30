from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc())
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "segment",
        "age",
        F.rank().over(w).alias("signup_date_rank"),
        F.last("country").over(w2).alias("last_country"),
    )
)
