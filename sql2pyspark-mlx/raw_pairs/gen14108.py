from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.lag("credit_limit").over(w_grp).alias("prev_credit_limit"),
        F.lead("age").over(w_grp).alias("next_age"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)
