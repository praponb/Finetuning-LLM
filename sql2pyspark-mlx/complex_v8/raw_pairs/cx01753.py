from pyspark.sql import functions as F

valid_adjusters = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%g %")) & (F.col("team") == "Property"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

adjuster_side = (
    valid_adjusters
    .filter(F.col("level") >= 3)
    .select("team", "id")
)

adjuster_rows = (
    df_adjusters
    .filter((F.col("team").isNotNull()) & (F.lower(F.col("adjuster_name")).like("%no%")))
    .select("team", "level")
)

adjuster_rows_2 = (
    df_adjusters
    .filter((F.col("level") <= 3) & (F.length(F.trim(F.col("adjuster_name"))) >= 5))
    .select("adjuster_name", "id")
)

result = (
    adjuster_side
    .union(adjuster_rows)
    .union(adjuster_rows_2)
)
