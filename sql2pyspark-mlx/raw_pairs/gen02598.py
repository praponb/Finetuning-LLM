from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "is_active",
        "age",
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lag("age").over(w).alias("prev_age"),
        F.lead("age").over(w).alias("next_age"),
        F.row_number().over(w).alias("seq_no"),
    )
)
