from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") > 3.5)
    .select(
        "id",
        "country",
        "city",
        "rating",
        "supplier_name",
        F.ntile(10).over(w).alias("rating_ntile"),
        F.first("supplier_name").over(w2).alias("first_supplier_name"),
    )
    .orderBy(F.col("id").desc())
)
