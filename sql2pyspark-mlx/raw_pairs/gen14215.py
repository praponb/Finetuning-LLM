from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "age",
        F.lead("age", 1, 0).over(w).alias("next_age"),
    )
)
