from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.lag("sqft").over(w_hist).alias("prev_sqft"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)
