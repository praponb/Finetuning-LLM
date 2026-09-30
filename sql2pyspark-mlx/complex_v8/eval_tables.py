"""Tables for `eval/evaluate.py --tables` (the complex_v8 catalog).

evaluate.py loads --tables files with importlib without registering them in sys.modules, which breaks
@dataclass on Python 3.14, so this wrapper (no dataclasses) imports catalog.py normally instead.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog import make_dataframes  # noqa: E402,F401
