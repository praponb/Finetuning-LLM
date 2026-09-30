from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Lio%"))
    .select(
        "id",
        "city",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
    )
)
