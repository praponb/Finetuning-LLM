from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select(
        "id",
        "segment",
        "city",
        "signup_date",
        "age",
        F.lead("age", 2).over(w).alias("next_age"),
    )
)
