from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment").orderBy("age", "id")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "credit_limit",
        F.lead("age").over(w_hist).alias("next_age"),
        F.lag("age").over(w_hist).alias("prev_age"),
        F.lead("credit_limit").over(w_hist).alias("next_credit_limit"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)
