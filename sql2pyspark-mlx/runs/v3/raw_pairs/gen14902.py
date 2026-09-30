from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy("age")

result = (
    df_customers
    .filter(F.col("name").like("N%"))
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        F.lead("age", 2).over(w).alias("next_age"),
        F.rank().over(w2).alias("age_rank"),
    )
)
