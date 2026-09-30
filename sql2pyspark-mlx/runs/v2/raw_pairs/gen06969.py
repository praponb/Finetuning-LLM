from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")
w2 = Window.partitionBy("country").orderBy("age")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "country",
        "age",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("rnk_2"),
    )
)
