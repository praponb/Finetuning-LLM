from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")
w2 = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.dense_rank().over(w2).alias("dense_rnk_2"),
    )
    .orderBy(F.col("id").desc())
)
