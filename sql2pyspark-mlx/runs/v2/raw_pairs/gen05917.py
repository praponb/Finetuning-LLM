from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "age",
        F.lead("age", 1).over(w).alias("next_age"),
    )
)
