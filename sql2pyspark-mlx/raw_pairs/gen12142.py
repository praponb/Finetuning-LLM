from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(3).over(w2).alias("rating_bucket"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)
