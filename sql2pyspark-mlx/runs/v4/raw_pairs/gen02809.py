from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("rating").between(2.5, 3.4))
    .select(
        "id",
        "country",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
    )
)
