from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "*",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)
