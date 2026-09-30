from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "age",
        F.lead("age", 2).over(w).alias("next_age"),
    )
    .orderBy("signup_date")
)
