from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.max("rating").over(w).alias("country_max_rating"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
)
